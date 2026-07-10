package User;

import Exceptions.DataValidationException;
import Exceptions.InvalidUserInput;
import Tradable.TradableDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class UserManager {
    private final ConcurrentMap<String, User> mngr = new ConcurrentHashMap<>();
    private static UserManager instance;

    private UserManager() {
    }

    public static synchronized UserManager getInstance() {
        if (instance == null) {
            instance = new UserManager();
        }
        return instance;
    }

    public void init(String[] usersIn) throws DataValidationException, InvalidUserInput {
        if (usersIn == null) {
            throw new DataValidationException("usersIn cannot be null");
        }

        for (String userId : usersIn) {
            if (userId == null) {
                throw new DataValidationException("userId cannot be null");
            }
            mngr.computeIfAbsent(userId, id -> {
                try {
                    return new User(id);
                } catch (InvalidUserInput e) {
                    throw new IllegalArgumentException(e);
                }
            });
        }
    }

    public User getRandomUser() {
        if (mngr.isEmpty()) {
            return null;
        }

        List<User> listOfUsers = new ArrayList<User>(mngr.values());
        Collections.shuffle(listOfUsers);
        return listOfUsers.get(0);
    }

    public void addToUser(String userId, TradableDTO o) throws DataValidationException {

        if (o == null) {
            throw new DataValidationException("TradableDTO is null");
        }

        if (userId == null) {
            throw new DataValidationException("userId is null");
        }

        User usr = mngr.get(userId);

        if (usr == null) {
            throw new DataValidationException("User is null.");
        }

        usr.addTradable(o);
    }

    public User getUser(String id) {
        if (mngr.get(id) == null) {
            return null;
        }
        return mngr.get(id);
    }

    public String toString() {
        for (User usr : mngr.values()) {
            System.out.println(usr);
        }
        return "";
    }

}
